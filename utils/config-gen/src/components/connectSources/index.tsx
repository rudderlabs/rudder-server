import { DottedCircle, IconCircle } from '@components/common/dottedCircle';
import { Flex } from '@components/common/misc';
import { HeaderDiv, TextDiv } from '@components/common/typography';
import IconCardList from '@components/iconCardList';
import EmptySourceCard from '@components/sourceCard/emptySourceCard';
import { IDestinationsListStore } from '@stores/destinationsList';
import { ISourceStore } from '@stores/source';
import { ISourcesListStore } from '@stores/sourcesList';
import Svg from '@svg/index';
import { inject, observer } from 'mobx-react';
import * as React from 'react';
import { RouteComponentProps, withRouter } from 'react-router';
import { withTheme } from 'styled-components';

import {
  AddDestDialogBody,
  IconCardListContainer,
  StyledContainer,
  BorderLine,
} from './styles';
import { ButtonSecondary } from '@components/common/button';

export interface IConnectSourcesProps extends RouteComponentProps<any> {
  destinationsListStore: IDestinationsListStore;
  sourcesListStore: ISourcesListStore;
  destinationDefId?: string;
  theme: any;
}

@inject('destinationsListStore', 'sourcesListStore')
@observer
class ConnectSources extends React.Component<IConnectSourcesProps, any> {
  constructor(props: IConnectSourcesProps) {
    super(props);
    const propsDestinationId = props.match.params.id;

    let destination: any = undefined;
    if (propsDestinationId) {
      var selectedDestination = props.destinationsListStore.destinations.find(
        destination => destination.id === propsDestinationId,
      );
      if (selectedDestination) {
        destination = selectedDestination;
      }
    }
    this.state = {
      selectedSources: [],
      destination,
      refinedSourceList: [],
    };
  }

  public componentDidMount() {
    const { sourcesListStore } = this.props;
    const { refinedSourceList, destination } = this.state;
    const destinationId = destination.id;
    const sourcesList = sourcesListStore.sources;

    for (let sourceCount = 0; sourceCount < sourcesList.length; sourceCount++) {
      var check = true;
      for (let i = 0; i < sourcesList[sourceCount].destinations.length; i++) {
        if (sourcesList[sourceCount].destinations[i].id === destinationId) {
          check = false;
        }
      }
      if (check === true) {
        refinedSourceList.push(sourcesList[sourceCount]);
      }
    }
    this.setState({ refinedSourceList });
  }

  public handleSourceSelection = (selectedMap: any) => {
    let sourceIds = Object.keys(selectedMap).filter(k => selectedMap[k]);
    if (sourceIds.length < 1) {
      return this.setState({ enableNextButton: false, selectedSources: [] });
    }
    this.setState({
      selectedSources: this.props.sourcesListStore.sources.filter(
        source => sourceIds.indexOf(source.id) > -1,
      ),
    });
  };

  public handleCancel = () => {
    if (this.props.history.length > 2) {
      this.props.history.goBack();
    } else {
      this.props.history.push('/connections');
    }
  };

  public handleSubmit = async () => {
    const { destination, selectedSources } = this.state;
    let destinationId = destination.id;
    const { destinationsListStore } = this.props;
    if (selectedSources.length > 0) {
      let selectedDestination = destinationsListStore.destinations.find(
        destination => destination.id === destinationId,
      );
      if (selectedDestination) {
        selectedDestination.sources.push(selectedSources);
      }
      await this.props.destinationsListStore.createDestinationConnections(
        selectedDestination,
        selectedSources.map((source: any) => source.id),
      );
      this.props.history.push(`/`);
    }
  };


  renderIconCardListContainer = () => {
    const { selectedSources, destination, refinedSourceList } = this.state;
    if (refinedSourceList.length > 0) {
      return (
        <IconCardListContainer>
          {this.props.sourcesListStore.sources.length === 0 ? (
            <Flex justifyContentCenter>
              <EmptySourceCard destinationId={destination.id} />
            </Flex>
          ) : (
            <IconCardList
              type="source"
              selectionMode="multi"
              destinationDefConfig={
                destination.destinationDefinition.config.sourceType
              }
              icons={refinedSourceList.map((source: any) => ({
                  id: source.id,
                  type: source.sourceDef.name,
                  title: source.name,
                  selected:
                    selectedSources.length > 0
                      ? source.id === selectedSources[0].id
                        ? true
                        : false
                      : false,
                }))}
              onSelectionChange={this.handleSourceSelection}
            />
          )}
        </IconCardListContainer>
      );
    }
    return (
      <IconCardListContainer>
        <Flex justifyContentCenter>
          <EmptySourceCard destinationId={destination.id} />
        </Flex>
      </IconCardListContainer>
    );
  };

  renderModalFooter = () => {
    const { selectedSources, destination, refinedSourceList } = this.state;
    if (refinedSourceList.length > 0) {
      return (
        <>
          <BorderLine />
          <Flex flexDirection="row" spaceBetween className="p-h-md p-v-xs">
            <ButtonSecondary onClick={this.handleCancel}>
              Cancel
            </ButtonSecondary>
            <ButtonSecondary onClick={this.handleSubmit}>
              Submit
            </ButtonSecondary>
          </Flex>
        </>
      );
    }
  };

  public render() {
    const { selectedSources, destination, refinedSourceList } = this.state;
    return (
      <StyledContainer>
        <HeaderDiv className="p-b-lg"></HeaderDiv>
        <AddDestDialogBody>
          <Flex justifyContentCenter className="p-t-lg" alignItems="center">
            <DottedCircle solid />
            <Flex alignItems="center">
              {this.state.selectedSources.map(
                (source: ISourceStore, index: number) => {
                  return (
                    <IconCircle
                      name={source.sourceDef.name}
                      listIndex={index}
                    />
                  );
                },
              )}
            </Flex>
            <div className="p-l-sm p-r-sm">
              <Svg name="forward-thick" />
            </div>
            <IconCircle
              name={destination.destinationDefinition.name}
              destination
            />
          </Flex>
          <HeaderDiv className="text-center p-t-md">Connect Sources</HeaderDiv>
          {this.renderIconCardListContainer()}
          {this.renderModalFooter()}
        </AddDestDialogBody>
      </StyledContainer>
    );
  }
}

export default withTheme(withRouter(ConnectSources));
