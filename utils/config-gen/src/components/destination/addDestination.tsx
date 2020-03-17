import { DottedCircle, IconCircle } from '@components/common/dottedCircle';
import { Input } from '@components/common/input';
import { Flex } from '@components/common/misc';
import { HeaderDiv, TextDiv } from '@components/common/typography';
import IconCardList from '@components/iconCardList';
import EmptySourceCard from '@components/sourceCard/emptySourceCard';
import Steps from '@components/steps';
import Step from '@components/steps/step';
import { IDestinationDefsListStore } from '@stores/destinationDefsList';
import { IDestinationsListStore } from '@stores/destinationsList';
import { ISourceStore } from '@stores/source';
import { ISourcesListStore } from '@stores/sourcesList';
import Svg from '@svg/index';
import { inject, observer } from 'mobx-react';
import queryString from 'query-string';
import * as React from 'react';
import { RouteComponentProps, withRouter } from 'react-router';
import { withTheme } from 'styled-components';
import formTemplatesMap from '../destination/destinationSettings/destinationSettings.json';

import {
  AddDestDialogBody,
  DestNameInputContainer,
  IconCardListContainer,
  StyledContainer,
  FormContainer,
  CenterDiv,
  FormBody,
} from './styles';
import DestinationSettings from './destinationSettings/destinationSettings';
import { ISourceDefintion } from '@stores/sourceDefinitionsList';

export interface IAddDestinationProps extends RouteComponentProps<any> {
  destinationDefsListStore: IDestinationDefsListStore;
  destinationsListStore: IDestinationsListStore;
  sourcesListStore: ISourcesListStore;
  destinationDefId?: string;
  theme: any;
}

@inject('destinationDefsListStore', 'destinationsListStore', 'sourcesListStore')
@observer
class AddDestination extends React.Component<IAddDestinationProps, any> {
  constructor(props: IAddDestinationProps) {
    super(props);
    const parsed = queryString.parse(this.props.location.search);

    let selectedSources: any = [];
    if (parsed.sourceId) {
      const selectedSource = props.sourcesListStore.sources.find(
        source => source.id === parsed.sourceId,
      );
      if (selectedSource) {
        selectedSources = [selectedSource];
      }
    }
    this.state = {
      currentStep: parsed.destinationDefId ? 1 : 0,
      enableNextButton: selectedSources ? true : false,
      destinationName: '',
      selectedDestDefintionId: parsed.destinationDefId || null,
      selectedDestDef:
        props.destinationDefsListStore.destinationDefs.filter(
          def => def.id === parsed.destinationDefId,
        )[0] || null,
      selectedSources,
      config: {},
      showNextLoader: false,
      filteredDestinationDefs: [],
    };
  }

  componentDidMount() {
    const { destinationDefsListStore } = this.props;

    if (destinationDefsListStore) {
      const destinationDefs = destinationDefsListStore!.destinationDefs;
      const destinationSettingsArr = Object.keys(formTemplatesMap);
      const filteredArr = [] as Array<object>;
      destinationDefs.map(def => {
        if (destinationSettingsArr.includes(def.name)) {
          filteredArr.push(def);
        }
      });
      this.setState({ filteredDestinationDefs: filteredArr });
    }
  }

  public enableConnection = async () => {
    const { destinationsListStore } = this.props;
    const {
      destinationName,
      config,
      selectedDestDef,
      selectedSources,
    } = this.state;
    const dest = await destinationsListStore.createDestination({
      name: destinationName,
      config: config,
      destinationDefinitionId: selectedDestDef.id,
    });
    await destinationsListStore.createDestinationConnections(
      dest,
      selectedSources.map((source: any) => source.id),
    );
    this.setState({ destinationId: dest.id });
  };

  public handleNext = async (event?: React.MouseEvent<HTMLElement>) => {
    const { currentStep } = this.state;
    if (currentStep === 3) {
      this.setState({ showNextLoader: true });
      await this.enableConnection();
      this.props.history.push(`/`);
    } else {
      this.setState({
        currentStep: currentStep + 1,
        enableNextButton: false,
      });
    }
  };

  public handleCancel = (event: React.MouseEvent<HTMLElement>) => {
    if (this.props.history.length > 2) {
      this.props.history.goBack();
    } else {
      this.props.history.push('/connections');
    }
  };

  public handleSelection = (selectedMap: any) => {
    const destDefId = Object.keys(selectedMap)[0];
    if (selectedMap[destDefId]) {
      this.setState({
        enableNextButton: true,
        selectedDestDefintionId: destDefId,
        selectedDestDef: this.state.filteredDestinationDefs.filter(
          (def: any) => def.id === destDefId,
        )[0],
      });
    } else {
      this.setState({
        enableNextButton: false,
        selectedDestDefintionId: null,
      });
    }
  };

  public handleNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    let enableNextButton = event.target.value.length > 0;
    this.setState({ destinationName: event.target.value, enableNextButton });
  };

  public handleSourceSelection = (selectedMap: any) => {
    let sourceIds = Object.keys(selectedMap).filter(k => selectedMap[k]);
    if (sourceIds.length < 1) {
      return this.setState({ enableNextButton: false, selectedSources: [] });
    }
    this.setState({
      enableNextButton: true,
      selectedSources: this.props.sourcesListStore.sources.filter(
        source => sourceIds.indexOf(source.id) > -1,
      ),
    });
  };

  public handleKeyDown = (e: any) => {
    if (e.key === 'Enter' && this.state.enableNextButton) {
      this.handleNext();
    }
  };

  public handleConfigChange = (config: any) => {
    this.setState({ config });
  };

  // public handleTransformationSelection = (
  //   transformation: ITransformationStore | null,
  // ) => {
  //   this.setState({ transformation });
  // };

  public render() {
    const {
      selectedSources,
      selectedDestDef,
      destinationId,
      filteredDestinationDefs,
    } = this.state;
    let icon;
    if (selectedSources.length > 0) {
      icon = selectedSources.map((source: any) => source.sourceDef.name);
    } else {
      icon = null;
    }
    return (
      <StyledContainer>
        <HeaderDiv className="p-b-lg">Add Destination</HeaderDiv>
        <Steps
          onCancel={this.handleCancel}
          onNext={this.handleNext}
          current={this.state.currentStep}
          enableNext={this.state.enableNextButton}
          showNextLoader={this.state.showNextLoader}
        >
          <Step>
            <AddDestDialogBody>
              <Flex justifyContentCenter className="p-t-lg" alignItems="center">
                {icon != null ? (
                  <DottedCircle iconName={icon} />
                ) : (
                  <DottedCircle />
                )}
                <div className="p-l-sm p-r-sm">
                  <Svg name="forward-thick" />
                </div>
                {selectedDestDef != null ? (
                  <DottedCircle iconName={selectedDestDef!.name} destination />
                ) : (
                  <DottedCircle solid />
                )}
              </Flex>
              <HeaderDiv className="text-center p-t-md">
                Choose a destination
              </HeaderDiv>
              <IconCardListContainer>
                <IconCardList
                  type="destination"
                  selectionMode="single"
                  icons={filteredDestinationDefs.map((def: any) => ({
                    id: def.id,
                    type: def.name,
                    title: def.displayName,
                  }))}
                  onSelectionChange={this.handleSelection}
                />
              </IconCardListContainer>
            </AddDestDialogBody>
          </Step>
          <Step>
            <AddDestDialogBody>
              <Flex justifyContentCenter className="p-t-lg" alignItems="center">
                {icon ? <DottedCircle iconName={icon} /> : <DottedCircle />}
                <div className="p-l-sm p-r-sm">
                  <Svg name="forward-thick" />
                </div>
                {selectedDestDef != null ? (
                  <DottedCircle iconName={selectedDestDef!.name} destination />
                ) : (
                  <DottedCircle solid />
                )}
              </Flex>
              <HeaderDiv className="text-center p-t-md">
                Name destination
              </HeaderDiv>
              <DestNameInputContainer>
                <Input
                  placeholder="eg. Google Analytics Dev"
                  onChange={this.handleNameChange}
                  autoFocus
                  onKeyDown={this.handleKeyDown}
                />
                <TextDiv
                  color={this.props.theme.color.grey300}
                  className="p-t-sm"
                >
                  Pick a name to help you identify this destination.
                </TextDiv>
              </DestNameInputContainer>
            </AddDestDialogBody>
          </Step>
          <Step>
            {this.state.selectedDestDef && (
              <AddDestDialogBody>
                <Flex
                  justifyContentCenter
                  className="p-t-lg"
                  alignItems="center"
                >
                  <DottedCircle solid />
                  <Flex className="selected-source-icons" alignItems="center">
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
                    name={this.state.selectedDestDef!.name}
                    destination
                  />
                </Flex>
                <HeaderDiv className="text-center p-t-md">
                  Connect Sources
                </HeaderDiv>
                <IconCardListContainer>
                  {this.props.sourcesListStore.sources.length === 0 ? (
                    <Flex justifyContentCenter>
                      <EmptySourceCard />
                    </Flex>
                  ) : (
                    <IconCardList
                      type="source"
                      selectionMode="multi"
                      destinationDefConfig={
                        this.state.selectedDestDef.config.sourceType
                      }
                      icons={this.props.sourcesListStore.sources.map(
                        source => ({
                          id: source.id,
                          type: source.sourceDef.name,
                          title: source.name,
                          selected:
                            selectedSources.length > 0
                              ? source.id === selectedSources[0].id
                                ? true
                                : false
                              : false,
                        }),
                      )}
                      onSelectionChange={this.handleSourceSelection}
                    />
                  )}
                </IconCardListContainer>
              </AddDestDialogBody>
            )}
          </Step>
          <Step>
            {this.state.selectedDestDef && (
              <FormBody>
                <Flex
                  justifyContentCenter
                  className="p-t-lg"
                  alignItems="center"
                >
                  <DottedCircle solid />
                  <Flex className="selected-source-icons" alignItems="center">
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
                    name={this.state.selectedDestDef!.name}
                    destination
                  />
                </Flex>
                <FormContainer>
                  <DestinationSettings
                    destName={this.state.selectedDestDef!.name}
                    onSettingsChange={this.handleConfigChange}
                    setRequirementsState={(reqsState: any) =>
                      this.setState({ enableNextButton: reqsState })
                    }
                  ></DestinationSettings>
                </FormContainer>
              </FormBody>
            )}
          </Step>
        </Steps>
      </StyledContainer>
    );
  }
}

export default withTheme(withRouter(AddDestination));
