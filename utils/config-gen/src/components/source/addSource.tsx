import { Input } from '@components/common/input';
import { HeaderDiv, TextDiv } from '@components/common/typography';
import IconCardList from '@components/iconCardList';
import Steps from '@components/steps';
import Step from '@components/steps/step';
import { ISourceDefinitionsListStore } from '@stores/sourceDefinitionsList';
import { ISourcesListStore } from '@stores/sourcesList';
import { IDestinationsListStore } from '@stores/destinationsList';
import { History } from 'history';
import { inject, observer } from 'mobx-react';
import queryString from 'query-string';
import * as React from 'react';
import { RouteComponentProps, withRouter } from 'react-router';
import { withTheme } from 'styled-components';
import {
  IconCardListContainer,
  SourceNameInputContainer,
  StyledContainer,
} from './styles';
import { IMessageStore } from '@stores/messages';

export interface IAddSourceProps extends RouteComponentProps<any> {
  sourceDefinitionsListStore: ISourceDefinitionsListStore;
  sourcesListStore: ISourcesListStore;
  destinationsListStore: IDestinationsListStore;
  messagesStore: IMessageStore;
  sourceDefId?: string;
  history: History;
  theme: any;
}

@inject(
  'sourceDefinitionsListStore',
  'sourcesListStore',
  'destinationsListStore',
  'messagesStore',
)
@observer
class AddSource extends React.Component<IAddSourceProps, any> {
  constructor(props: IAddSourceProps) {
    super(props);
    const parsed = queryString.parse(this.props.location.search);
    this.state = {
      ref: (props.match && props.match.params.id) || null,
      currentStep: parsed.sourceDefId ? 1 : 0,
      enableNextButton: false,
      sourceName: '',
      selectedSourceDefintionId: parsed.sourceDefId || null,
      showNextLoader: false,
      filteredSources: [],
    };
  }

  public handleCancel = (event: React.MouseEvent<HTMLElement>) => {
    if (this.props.history.length > 2) {
      this.props.history.goBack();
    } else {
      this.props.history.push('/');
    }
  };

  async componentDidMount() {
    const filteredSources = await this.props.sourceDefinitionsListStore.getFilteredSourceDefinitions();
    this.setState({ filteredSources });
  }

  createConnection = async (source: any) => {
    const { ref } = this.state;
    const { destinationsListStore } = this.props;

    if (ref != null) {
      const destination = destinationsListStore.destinations.find(
        dest => dest.id === ref,
      );
      await destinationsListStore.createDestinationConnections(destination, [
        source.id,
      ]);
    }
  };

  public handleNext = async (event?: React.MouseEvent<HTMLElement>) => {
    try {
      if (this.state.currentStep === 1) {
        this.setState({ showNextLoader: true });
        const source = await this.props.sourcesListStore.createSource({
          name: this.state.sourceName,
          sourceDefinitionId: this.state.selectedSourceDefintionId,
        });
        this.props.messagesStore.showSuccessMessage('Added source');
        if (this.state.ref != null) {
          this.createConnection(source);
          this.props.history.push(`/destinations/${this.state.ref}`);
        } else {
          this.props.history.push(`/sources/${source.id}`);
        }
      } else {
        this.setState({
          currentStep: this.state.currentStep + 1,
          enableNextButton: false,
        });
      }
    } catch (error) {
      this.setState({ showNextLoader: false });
      this.props.messagesStore.showErrorMessage(
        'Failed to add source. Try again after sometime',
      );
      //throw Error(error); // ToDo how will bugsnag error-boundry know of this?
    }
  };

  public handleSelection = (selectedMap: any) => {
    const sourceDefId = Object.keys(selectedMap)[0];
    if (selectedMap[sourceDefId]) {
      this.setState({
        enableNextButton: true,
        selectedSourceDefintionId: sourceDefId,
      });
    } else {
      this.setState({
        enableNextButton: false,
        selectedSourceDefintionId: null,
      });
    }
  };

  public handleNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    let enableNextButton = event.target.value.length > 0;
    this.setState({ sourceName: event.target.value, enableNextButton });
  };

  public handleKeyDown = (e: any) => {
    if (e.key === 'Enter' && this.state.enableNextButton) {
      this.handleNext();
    }
  };

  public render() {
    const { filteredSources } = this.state;
    if (filteredSources.length > 0) {
      return (
        <StyledContainer>
          <HeaderDiv className="p-b-lg">Add Source</HeaderDiv>
          <Steps
            onCancel={this.handleCancel}
            onNext={this.handleNext}
            current={this.state.currentStep}
            enableNext={this.state.enableNextButton}
            showNextLoader={this.state.showNextLoader}
          >
            <Step>
              <HeaderDiv className="text-center p-t-lg">
                Choose a source
              </HeaderDiv>
              <IconCardListContainer>
                <IconCardList
                  type="source"
                  selectionMode="single"
                  icons={filteredSources.map((s: any) => ({
                    id: s.id,
                    type: s.name,
                    title: s.name,
                  }))}
                  onSelectionChange={this.handleSelection}
                />
              </IconCardListContainer>
            </Step>
            <Step>
              <HeaderDiv className="text-center p-t-lg">
                Name your source
              </HeaderDiv>
              <SourceNameInputContainer>
                <Input
                  placeholder="eg. Android Dev"
                  onChange={this.handleNameChange}
                  autoFocus
                  onKeyDown={this.handleKeyDown}
                />
                <TextDiv
                  color={this.props.theme.color.grey300}
                  className="p-t-sm"
                >
                  Identifies this source within your workspace, and typically
                  includes the product area and environment.
                </TextDiv>
              </SourceNameInputContainer>
            </Step>
          </Steps>
        </StyledContainer>
      );
    } else {
      return <div></div>;
    }
  }
}

export default withTheme(withRouter(AddSource));
